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
      inputs: agg.s.period, agg.tot
      - SelectNode source=select
        outputs: agg.s.period, agg.tot
        inputs: local._agg_tot, s.period
        - GroupNode source=group
          outputs: local._agg_tot, s.period
          inputs: s.amt, s.period
          - SelectNode source=select datasource=s.sales
            outputs: s.amt, s.period, s.sid
            inputs: s.amt, s.period, s.region, s.sid
    - GroupNode source=group
      outputs: agg.s.period, local._virt_agg_sum_9228488784072447
      inputs: agg.s.period, fut.tot
      - MergeNode source=merge
        outputs: agg.s.period, agg.tot, fut.s.period, fut.tot
        inputs: agg.s.period, agg.tot, fut.s.period, fut.tot
        - SelectNode source=select
          outputs: agg.s.period, fut.s.period, fut.tot
          inputs: local._fut_tot, s.period
          - GroupNode source=group
            outputs: local._fut_tot, s.period
            inputs: s.amt, s.period
            - SelectNode source=select datasource=s.sales
              outputs: s.amt, s.period, s.sid
              inputs: s.amt, s.period, s.region, s.sid
        - SelectNode source=select
          outputs: agg.s.period, agg.tot, fut.s.period
          inputs: local._agg_tot, s.period
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
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n3",
    "inputs": [
      "agg.s.period",
      "agg.tot"
    ],
    "nullable": [],
    "outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063"
    ],
    "parents": [
      "n2"
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
    "datasource": "s.sales",
    "existence": [],
    "force_group": false,
    "grain": "Grain<s.sid>",
    "hidden": [],
    "id": "n4",
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
    "id": "n5",
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
      "n4"
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
    "id": "n6",
    "inputs": [
      "s.period",
      "local._fut_tot"
    ],
    "nullable": [],
    "outputs": [
      "fut.s.period",
      "fut.tot",
      "agg.s.period"
    ],
    "parents": [
      "n5"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "select",
    "type": "SelectNode",
    "usable_outputs": [
      "fut.s.period",
      "fut.tot",
      "agg.s.period"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": false,
    "grain": "Grain<agg.s.period>",
    "hidden": [],
    "id": "n7",
    "inputs": [
      "s.period",
      "local._agg_tot"
    ],
    "nullable": [],
    "outputs": [
      "agg.s.period",
      "agg.tot",
      "fut.s.period"
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
      "agg.tot",
      "fut.s.period"
    ]
  },
  {
    "conditions": null,
    "datasource": null,
    "existence": [],
    "force_group": null,
    "grain": null,
    "hidden": [],
    "id": "n8",
    "inputs": [
      "fut.s.period",
      "fut.tot",
      "agg.s.period",
      "agg.tot"
    ],
    "nullable": [],
    "outputs": [
      "fut.s.period",
      "fut.tot",
      "agg.s.period",
      "agg.tot"
    ],
    "parents": [
      "n6",
      "n7"
    ],
    "partials": [],
    "preexisting_conditions": null,
    "rollups": [],
    "source_type": "merge",
    "type": "MergeNode",
    "usable_outputs": [
      "fut.s.period",
      "fut.tot",
      "agg.s.period",
      "agg.tot"
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
      "agg.s.period"
    ],
    "nullable": [],
    "outputs": [
      "agg.s.period",
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
      "agg.s.period",
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
    "nullable": [],
    "outputs": [
      "agg.s.period",
      "local._virt_agg_sum_4149934679595063",
      "local._virt_agg_sum_9228488784072447"
    ],
    "parents": [
      "n3",
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
