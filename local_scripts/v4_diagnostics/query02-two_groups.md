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
- predecessors: `grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq:merge, grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:merge, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:merge, grp:root:root:∅:merge, grp:root:root_d1:∅:merge, grp:window:d0:date.week_seq:merge`
- successors: `-`
- conditions: `BuildSubselectComparison(left=date.week_seq@Grain<date.id>, right=local.relevent_week_seq@Grain<date.id>, operator=<ComparisonOperator.IN: 'in'>)`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq",
      "output_addresses": [
        "date.week_seq",
        "local.relevent_week_seq"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id",
      "output_addresses": [
        "local._virt_agg_sum_2131371712943644",
        "local._virt_agg_sum_733654448721027",
        "date.week_seq",
        "local._virt_agg_sum_3727603509126659",
        "local._virt_agg_sum_5446384850356435",
        "local._virt_agg_sum_4518379598128005",
        "local._virt_agg_sum_7662941318754865",
        "local._virt_agg_sum_7224794219444244"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "date.week_seq"
      ]
    },
    {
      "group_id": "grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number",
      "output_addresses": [
        "local._virt_agg_sum_9238538473336606",
        "local._virt_agg_sum_6823422966658347",
        "local._virt_agg_sum_8302396398525202",
        "local._virt_agg_sum_8086371301050807",
        "date.week_seq",
        "local._virt_agg_sum_5332872165953971",
        "local._virt_agg_sum_2293560889657522",
        "local._virt_agg_sum_8833754379564371"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "date.week_seq"
      ]
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "catalog_sales.item.id",
        "web_sales.order_number",
        "date.day_of_week",
        "catalog_sales.order_number",
        "catalog_sales.ext_sales_price",
        "date.week_seq",
        "date.id",
        "web_sales.ext_sales_price",
        "web_sales.item.id"
      ],
      "preserve_keys": [
        "date.week_seq"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root_d1:\u2205",
      "output_addresses": [
        "date.id",
        "date.week_seq",
        "date.year"
      ],
      "preserve_keys": [
        "date.week_seq"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:window:d0:date.week_seq",
      "output_addresses": [
        "local.wednesday_increase",
        "local.saturday_increase",
        "local.friday_increase",
        "local.monday_increase",
        "date.week_seq",
        "local.sunday_increase",
        "local.thursday_increase",
        "local.tuesday_increase"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "date.week_seq"
      ]
    }
  ],
  "deduplicate_to_grain": true,
  "merge_grain": [
    "date.week_seq"
  ],
  "output_addresses": [
    "local.wednesday_increase",
    "local.saturday_increase",
    "local.friday_increase",
    "local.monday_increase",
    "date.week_seq",
    "local.sunday_increase",
    "local.thursday_increase",
    "local.tuesday_increase"
  ],
  "required_grain": [
    "date.week_seq"
  ]
}
```

## grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq

- derivation: `filter`
- depth: `d1`
- grain: `date.id`
- aggregate input grain: `-`
- primary members: `local.relevent_week_seq`
- secondary members: `-`
- outputs: `date.week_seq, local.relevent_week_seq`
- inputs: `date.id, date.week_seq, date.year`
- hidden: `-`
- predecessors: `grp:root:root_d1:∅:lineage`
- successors: `__final__:merge, grp:root:root:∅:existence`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root_d1:\u2205",
    "preserve_keys": [
      "date.id"
    ],
    "required_grain": [
      "date.id"
    ],
    "required_outputs": [
      "date.id",
      "date.week_seq",
      "date.year"
    ]
  }
]
```

## grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id

- derivation: `aggregate`
- depth: `d0`
- grain: `date.week_seq`
- aggregate input grain: `catalog_sales.item.id, catalog_sales.order_number, date.id`
- primary members: `local._virt_agg_sum_2131371712943644, local._virt_agg_sum_3727603509126659, local._virt_agg_sum_4518379598128005, local._virt_agg_sum_5446384850356435, local._virt_agg_sum_7224794219444244, local._virt_agg_sum_733654448721027, local._virt_agg_sum_7662941318754865`
- secondary members: `date.week_seq`
- outputs: `date.week_seq, local._virt_agg_sum_2131371712943644, local._virt_agg_sum_3727603509126659, local._virt_agg_sum_4518379598128005, local._virt_agg_sum_5446384850356435, local._virt_agg_sum_7224794219444244, local._virt_agg_sum_733654448721027, local._virt_agg_sum_7662941318754865`
- inputs: `catalog_sales.ext_sales_price, catalog_sales.item.id, catalog_sales.order_number, date.day_of_week, date.id, date.week_seq`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:window:d0:date.week_seq:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "catalog_sales.item.id",
      "date.day_of_week",
      "catalog_sales.ext_sales_price",
      "catalog_sales.order_number",
      "date.week_seq",
      "date.id"
    ]
  }
]
```

## grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number

- derivation: `aggregate`
- depth: `d0`
- grain: `date.week_seq`
- aggregate input grain: `date.id, web_sales.item.id, web_sales.order_number`
- primary members: `local._virt_agg_sum_2293560889657522, local._virt_agg_sum_5332872165953971, local._virt_agg_sum_6823422966658347, local._virt_agg_sum_8086371301050807, local._virt_agg_sum_8302396398525202, local._virt_agg_sum_8833754379564371, local._virt_agg_sum_9238538473336606`
- secondary members: `date.week_seq`
- outputs: `date.week_seq, local._virt_agg_sum_2293560889657522, local._virt_agg_sum_5332872165953971, local._virt_agg_sum_6823422966658347, local._virt_agg_sum_8086371301050807, local._virt_agg_sum_8302396398525202, local._virt_agg_sum_8833754379564371, local._virt_agg_sum_9238538473336606`
- inputs: `date.day_of_week, date.id, date.week_seq, web_sales.ext_sales_price, web_sales.item.id, web_sales.order_number`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:window:d0:date.week_seq:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "web_sales.order_number",
      "date.day_of_week",
      "date.week_seq",
      "date.id",
      "web_sales.ext_sales_price",
      "web_sales.item.id"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `catalog_sales.ext_sales_price, date.day_of_week, date.week_seq, web_sales.ext_sales_price`
- secondary members: `-`
- outputs: `catalog_sales.ext_sales_price, catalog_sales.item.id, catalog_sales.order_number, date.day_of_week, date.id, date.week_seq, web_sales.ext_sales_price, web_sales.item.id, web_sales.order_number`
- inputs: `catalog_sales.item.id, catalog_sales.order_number, date.id, web_sales.item.id, web_sales.order_number`
- hidden: `-`
- predecessors: `grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq:existence`
- successors: `__final__:merge, grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:lineage, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:lineage, grp:window:d0:date.week_seq:lineage`
- conditions: `BuildSubselectComparison(left=date.week_seq@Grain<date.id>, right=local.relevent_week_seq@Grain<date.id>, operator=<ComparisonOperator.IN: 'in'>)`
- input contracts:
```json
[
  {
    "channel": "existence",
    "consumer_group_id": "grp:root:root:\u2205",
    "may_project_dimension": false,
    "parent_group_id": "grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq",
    "preserve_keys": [],
    "required_grain": [],
    "required_outputs": []
  }
]
```

## grp:root:root_d1:∅

- derivation: `root`
- depth: `root_d1`
- grain: `-`
- aggregate input grain: `-`
- primary members: `date.id, date.week_seq, date.year`
- secondary members: `-`
- outputs: `date.id, date.week_seq, date.year`
- inputs: `-`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq:lineage`

## grp:window:d0:date.week_seq

- derivation: `window`
- depth: `d0`
- grain: `date.week_seq`
- aggregate input grain: `-`
- primary members: `local._virt_window_lead_1615489443759951, local._virt_window_lead_1732363590168359, local._virt_window_lead_4790424210530227, local._virt_window_lead_7125692363367989, local._virt_window_lead_9386088415621209, local._virt_window_lead_9762136461725141, local._virt_window_lead_9976629776715537, local.friday_increase, local.monday_increase, local.saturday_increase, local.sunday_increase, local.thursday_increase, local.tuesday_increase, local.wednesday_increase`
- secondary members: `date.week_seq`
- outputs: `date.week_seq, local.friday_increase, local.monday_increase, local.saturday_increase, local.sunday_increase, local.thursday_increase, local.tuesday_increase, local.wednesday_increase`
- inputs: `date.week_seq, local._virt_agg_sum_2131371712943644, local._virt_agg_sum_2293560889657522, local._virt_agg_sum_3727603509126659, local._virt_agg_sum_4518379598128005, local._virt_agg_sum_5332872165953971, local._virt_agg_sum_5446384850356435, local._virt_agg_sum_6823422966658347, local._virt_agg_sum_7224794219444244, local._virt_agg_sum_733654448721027, local._virt_agg_sum_7662941318754865, local._virt_agg_sum_8086371301050807, local._virt_agg_sum_8302396398525202, local._virt_agg_sum_8833754379564371, local._virt_agg_sum_9238538473336606`
- hidden: `-`
- predecessors: `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:lineage, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:lineage, grp:root:root:∅:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:window:d0:date.week_seq",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "local._virt_agg_sum_2131371712943644",
      "local._virt_agg_sum_733654448721027",
      "date.week_seq",
      "local._virt_agg_sum_3727603509126659",
      "local._virt_agg_sum_5446384850356435",
      "local._virt_agg_sum_4518379598128005",
      "local._virt_agg_sum_7662941318754865",
      "local._virt_agg_sum_7224794219444244"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:window:d0:date.week_seq",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "local._virt_agg_sum_9238538473336606",
      "local._virt_agg_sum_6823422966658347",
      "local._virt_agg_sum_8302396398525202",
      "local._virt_agg_sum_8086371301050807",
      "date.week_seq",
      "local._virt_agg_sum_5332872165953971",
      "local._virt_agg_sum_2293560889657522",
      "local._virt_agg_sum_8833754379564371"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:window:d0:date.week_seq",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "date.week_seq"
    ]
  }
]
```

# Edges

- `grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq` -> `__final__` kind=merge phase=pre_condition
- `grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq` -> `grp:root:root:∅` kind=existence phase=pre_condition
- `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` -> `__final__` kind=merge phase=post_condition
- `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` -> `grp:window:d0:date.week_seq` kind=lineage phase=post_condition
- `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` -> `__final__` kind=merge phase=post_condition
- `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` -> `grp:window:d0:date.week_seq` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `__final__` kind=merge phase=post_condition
- `grp:root:root:∅` -> `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` kind=lineage phase=post_condition
- `grp:root:root:∅` -> `grp:window:d0:date.week_seq` kind=lineage phase=post_condition
- `grp:root:root_d1:∅` -> `__final__` kind=merge phase=pre_condition
- `grp:root:root_d1:∅` -> `grp:[@condition]filter:d1:date.id:existence:local.relevent_week_seq` kind=lineage phase=pre_condition
- `grp:window:d0:date.week_seq` -> `__final__` kind=merge phase=post_condition
