# V4 Strategy Diagnostics

- strategy nodes: 7

## Tree

- MergeNode source=merge
  outputs: local.id
  inputs: local.half, local.id, local.val
  conditions: local.val > local.half
  - SelectNode source=select datasource=t
    outputs: local.id
    inputs: local.cat, local.id, local.val
  - MergeNode source=merge
    outputs: local.half, local.id, local.val
    inputs: local.half, local.id, local.val
    - SelectNode source=select
      outputs: local.half
      inputs: local.mx
      - GroupNode source=group
        outputs: local.mx
        inputs: local.val
        - SelectNode source=select datasource=t
          outputs: local.id, local.val
          inputs: local.cat, local.id, local.val
    - SelectNode source=select datasource=t
      outputs: local.id, local.val
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
      "local.mx"
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
      "local.mx"
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
      "local.mx"
    ],
    "nullable": [],
    "outputs": [
      "local.half"
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
      "local.half"
    ]
  },
  {
    "conditions": null,
    "datasource": "t",
    "existence": [],
    "force_group": false,
    "grain": "Grain<local.id>",
    "hidden": [],
    "id": "n4",
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
    "id": "n5",
    "inputs": [
      "local.half",
      "local.id",
      "local.val"
    ],
    "nullable": [
      "local.half",
      "local.id",
      "local.val"
    ],
    "outputs": [
      "local.half",
      "local.id",
      "local.val"
    ],
    "parents": [
      "n3",
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "local.half",
      "local.id",
      "local.val"
    ]
  },
  {
    "conditions": "local.val > local.half",
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n6",
    "inputs": [
      "local.id",
      "local.val",
      "local.half"
    ],
    "nullable": [
      "local.id"
    ],
    "outputs": [
      "local.id"
    ],
    "parents": [
      "n0",
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": "local.val > local.half",
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "local.id"
    ]
  }
]
```
