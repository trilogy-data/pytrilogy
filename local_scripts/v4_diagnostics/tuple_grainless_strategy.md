# V4 Strategy Diagnostics

- strategy nodes: 5

## Tree

- GroupNode source=group
  outputs: local.cross_pair_absent, local.present
  inputs: local.cross_pair_absent, local.present
  - SelectNode source=select
    outputs: local.cross_pair_absent, local.present
    inputs: pairs.cat, pairs.val
    - SelectNode source=select
      outputs: pairs.cat, pairs.val
      inputs: local.cat, local.val
      - GroupNode source=group
        outputs: local.cat, local.val
        inputs: local.cat, local.val
        - SelectNode source=select datasource=t
          outputs: local.cat, local.val
          inputs: local.cat, local.id, local.val
          conditions: local.val = 20

## Records

```json
[
  {
    "conditions": "local.val = 20",
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
      "local.cat",
      "local.val"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": "local.val = 20",
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "local.cat",
      "local.val"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": true,
    "grain": null,
    "hidden": [],
    "id": "n1",
    "inputs": [
      "local.cat",
      "local.val"
    ],
    "nullable": [],
    "outputs": [
      "local.cat",
      "local.val"
    ],
    "parents": [
      "n0"
    ],
    "partials": [],
    "preexisting_conditions": "local.val = 20",
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.cat",
      "local.val"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<pairs.cat,pairs.val>",
    "hidden": [],
    "id": "n2",
    "inputs": [
      "local.cat",
      "local.val"
    ],
    "nullable": [],
    "outputs": [
      "pairs.cat",
      "pairs.val"
    ],
    "parents": [
      "n1"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "pairs.cat",
      "pairs.val"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [
      "pairs.val",
      "pairs.cat"
    ],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n3",
    "inputs": [
      "pairs.cat",
      "pairs.val"
    ],
    "nullable": [],
    "outputs": [
      "local.cross_pair_absent",
      "local.present"
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
      "local.cross_pair_absent",
      "local.present"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n4",
    "inputs": [
      "local.cross_pair_absent",
      "local.present"
    ],
    "nullable": [],
    "outputs": [
      "local.cross_pair_absent",
      "local.present"
    ],
    "parents": [
      "n3"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local.cross_pair_absent",
      "local.present"
    ]
  }
]
```
