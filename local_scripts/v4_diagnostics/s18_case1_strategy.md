# V4 Strategy Diagnostics

- strategy nodes: 12

## Tree

- SelectNode source=select
  outputs: agg.s.period, local.r
  inputs: agg.s.period, local._virt_agg_sum_4149934679595063, local._virt_agg_sum_9228488784072447
  - MergeNode source=merge
    outputs: agg.s.period, local._virt_agg_sum_4149934679595063, local._virt_agg_sum_9228488784072447
    inputs: agg.s.period, local._virt_agg_sum_4149934679595063, local._virt_agg_sum_9228488784072447
    - GroupNode source=group
      outputs: agg.s.period, local._virt_agg_sum_4149934679595063
      inputs: agg.s.period, agg.tot, local._virt_func_add_8165249766077962
      - MergeNode source=merge
        outputs: agg.s.period, agg.tot, fut.s.period, fut.tot, local._virt_func_add_8165249766077962
        inputs: agg.s.period, agg.tot, fut.s.period, fut.tot, local._virt_func_add_8165249766077962
        - SelectNode source=select
          outputs: agg.s.period, agg.tot
          inputs: local._agg_tot, s.period
          - GroupNode source=group
            outputs: local._agg_tot, s.period
            inputs: s.amt, s.period
            - SelectNode source=select datasource=s.sales
              outputs: s.amt, s.period, s.sid
              inputs: s.amt, s.period, s.region, s.sid
        - SelectNode source=select
          outputs: fut.s.period, fut.tot, local._virt_func_add_8165249766077962
          inputs: local._fut_tot, s.period
          - GroupNode source=group
            outputs: local._fut_tot, s.period
            inputs: s.amt, s.period
            - SelectNode source=select datasource=s.sales
              outputs: s.amt, s.period, s.sid
              inputs: s.amt, s.period, s.region, s.sid
    - GroupNode source=group
      outputs: local._virt_agg_sum_9228488784072447
      inputs: fut.tot, local._virt_func_add_8165249766077962
      - SelectNode source=select
        outputs: fut.s.period, fut.tot, local._virt_func_add_8165249766077962
        inputs: local._fut_tot, s.period
        - GroupNode source=group (reused)

## Records

```json
[
  {
    "conditions": null,
    "datasource": "s.sales",
    "existence": [],
    "force_group": false,
    "grain": "Grain<s.sid>",
    "hidden": [],
    "id": "n0",
    "inputs": [
      "s.sid",
      "s.period",
      "s.region",
      "s.amt"
    ],
    "nullable": [],
    "outputs": [
      "s.amt",
      "s.period",
      "s.sid"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "s.amt",
      "s.period",
      "s.sid"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n1",
    "inputs": [
      "s.amt",
      "s.period"
    ],
    "nullable": [],
    "outputs": [
      "local._agg_tot",
      "s.period"
    ],
    "parents": [
      "n0"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local._agg_tot",
      "s.period"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<agg.s.period>",
    "hidden": [],
    "id": "n2",
    "inputs": [
      "s.period",
      "local._agg_tot"
    ],
    "nullable": [],
    "outputs": [
      "agg.s.period",
      "agg.tot"
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
      "agg.s.period",
      "agg.tot"
    ]
  },
  {
    "conditions": null,
    "datasource": "s.sales",
    "existence": [],
    "force_group": false,
    "grain": "Grain<s.sid>",
    "hidden": [],
    "id": "n3",
    "inputs": [
      "s.sid",
      "s.period",
      "s.region",
      "s.amt"
    ],
    "nullable": [],
    "outputs": [
      "s.amt",
      "s.period",
      "s.sid"
    ],
    "parents": [],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "s.amt",
      "s.period",
      "s.sid"
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
      "s.amt",
      "s.period"
    ],
    "nullable": [],
    "outputs": [
      "local._fut_tot",
      "s.period"
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
      "local._fut_tot",
      "s.period"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<fut.s.period>",
    "hidden": [],
    "id": "n5",
    "inputs": [
      "s.period",
      "local._fut_tot"
    ],
    "nullable": [],
    "outputs": [
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "parents": [
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n6",
    "inputs": [
      "agg.s.period",
      "agg.tot",
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "nullable": [
      "agg.s.period",
      "agg.tot",
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "outputs": [
      "agg.s.period",
      "agg.tot",
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "parents": [
      "n2",
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "agg.s.period",
      "agg.tot",
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n7",
    "inputs": [
      "agg.s.period",
      "agg.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "nullable": [
      "agg.s.period"
    ],
    "outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063"
    ],
    "parents": [
      "n6"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<fut.s.period>",
    "hidden": [],
    "id": "n8",
    "inputs": [
      "s.period",
      "local._fut_tot"
    ],
    "nullable": [],
    "outputs": [
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "parents": [
      "n4"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "fut.s.period",
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n9",
    "inputs": [
      "fut.tot",
      "local._virt_func_add_8165249766077962"
    ],
    "nullable": [],
    "outputs": [
      "local._virt_agg_sum_9228488784072447"
    ],
    "parents": [
      "n8"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "group",
    "type": "GroupNode",
    "usable_outputs": [
      "local._virt_agg_sum_9228488784072447"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n10",
    "inputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ],
    "nullable": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ],
    "outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ],
    "parents": [
      "n7",
      "n9"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": null,
    "hidden": [],
    "id": "n11",
    "inputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ],
    "nullable": [],
    "outputs": [
      "agg.s.period",
      "local.r"
    ],
    "parents": [
      "n10"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "agg.s.period",
      "local.r"
    ]
  }
]
```
