# V4 Group Diagnostics

- groups: 8
- edges: 16

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
- predecessors: `grp:[@condition]filter:d1:date.id:sig:9dfe8e:merge, grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:merge, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:merge, grp:basic:d*:date.id:sig:d8f8fc:merge, grp:basic:d*:date.week_seq:sig:bf8c7f:merge, grp:root:root:∅:merge, grp:window:d0:date.week_seq:merge`
- successors: `-`
- conditions: `BuildSubselectComparison(left=date.week_seq@Grain<date.id>, right=local.relevent_week_seq@Grain<date.id>, operator=<ComparisonOperator.IN: 'in'>)`
- final contract:
```json
{
  "contributor_contracts": [
    {
      "group_id": "grp:[@condition]filter:d1:date.id:sig:9dfe8e",
      "output_addresses": [
        "date.week_seq",
        "local.relevent_week_seq",
        "date.id"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id",
      "output_addresses": [
        "date.week_seq",
        "local._virt_agg_sum_7662941318754865",
        "local._virt_agg_sum_3727603509126659",
        "local._virt_agg_sum_5446384850356435",
        "local._virt_agg_sum_7224794219444244",
        "local._virt_agg_sum_2131371712943644",
        "local._virt_agg_sum_733654448721027",
        "local._virt_agg_sum_4518379598128005"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "date.week_seq"
      ]
    },
    {
      "group_id": "grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number",
      "output_addresses": [
        "date.week_seq",
        "local._virt_agg_sum_6823422966658347",
        "local._virt_agg_sum_8086371301050807",
        "local._virt_agg_sum_5332872165953971",
        "local._virt_agg_sum_8302396398525202",
        "local._virt_agg_sum_2293560889657522",
        "local._virt_agg_sum_9238538473336606",
        "local._virt_agg_sum_8833754379564371"
      ],
      "preserve_keys": [],
      "projection_grain": [
        "date.week_seq"
      ]
    },
    {
      "group_id": "grp:basic:d*:date.id:sig:d8f8fc",
      "output_addresses": [
        "local.monday_increase",
        "date.week_seq",
        "local.sunday_increase",
        "local.tuesday_increase",
        "local.saturday_increase",
        "local.friday_increase",
        "local.thursday_increase",
        "local.wednesday_increase"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:basic:d*:date.week_seq:sig:bf8c7f",
      "output_addresses": [
        "date.week_seq",
        "local.monday_sales",
        "local.tuesday_sales",
        "local.thursday_sales",
        "local.friday_sales",
        "local.sunday_sales",
        "local.saturday_sales",
        "local.wednesday_sales"
      ],
      "preserve_keys": [],
      "projection_grain": []
    },
    {
      "group_id": "grp:root:root:\u2205",
      "output_addresses": [
        "date.week_seq",
        "catalog_sales.ext_sales_price",
        "catalog_sales.order_number",
        "catalog_sales.item.id",
        "date.day_of_week",
        "date.year",
        "web_sales.order_number",
        "web_sales.ext_sales_price",
        "web_sales.item.id",
        "date.id"
      ],
      "preserve_keys": [
        "date.week_seq"
      ],
      "projection_grain": []
    },
    {
      "group_id": "grp:window:d0:date.week_seq",
      "output_addresses": [
        "date.week_seq",
        "local._virt_window_lead_9809563217267406",
        "local._virt_window_lead_5767709179323528",
        "local._virt_window_lead_2497781736791521",
        "local._virt_window_lead_5067641372653397",
        "local._virt_window_lead_3949607449893123",
        "local._virt_window_lead_6651551761445993",
        "local._virt_window_lead_315641373767519"
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
    "local.monday_increase",
    "date.week_seq",
    "local.sunday_increase",
    "local.tuesday_increase",
    "local.saturday_increase",
    "local.friday_increase",
    "local.thursday_increase",
    "local.wednesday_increase"
  ],
  "required_grain": [
    "date.week_seq"
  ]
}
```

## grp:[@condition]filter:d1:date.id:sig:9dfe8e

- derivation: `filter`
- depth: `d1`
- grain: `date.id`
- aggregate input grain: `-`
- primary members: `local.relevent_week_seq`
- secondary members: `-`
- outputs: `date.id, date.week_seq, local.relevent_week_seq`
- inputs: `date.id, date.week_seq, date.year`
- hidden: `-`
- predecessors: `grp:root:root:∅:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:[@condition]filter:d1:date.id:sig:9dfe8e",
    "may_project_dimension": true,
    "parent_group_id": "grp:root:root:\u2205",
    "preserve_keys": [
      "date.id"
    ],
    "required_grain": [
      "date.id"
    ],
    "required_outputs": [
      "date.week_seq",
      "date.id",
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
- successors: `__final__:merge, grp:basic:d*:date.week_seq:sig:bf8c7f:lineage`
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
      "date.week_seq",
      "catalog_sales.ext_sales_price",
      "catalog_sales.order_number",
      "catalog_sales.item.id",
      "date.day_of_week",
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
- successors: `__final__:merge, grp:basic:d*:date.week_seq:sig:bf8c7f:lineage`
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
      "date.week_seq",
      "date.day_of_week",
      "web_sales.order_number",
      "web_sales.ext_sales_price",
      "web_sales.item.id",
      "date.id"
    ]
  }
]
```

## grp:basic:d*:date.id:sig:d8f8fc

- derivation: `basic`
- depth: `d*`
- grain: `date.id`
- aggregate input grain: `-`
- primary members: `local.friday_increase, local.monday_increase, local.saturday_increase, local.sunday_increase, local.thursday_increase, local.tuesday_increase, local.wednesday_increase`
- secondary members: `-`
- outputs: `date.week_seq, local.friday_increase, local.monday_increase, local.saturday_increase, local.sunday_increase, local.thursday_increase, local.tuesday_increase, local.wednesday_increase`
- inputs: `date.week_seq, local._virt_window_lead_2497781736791521, local._virt_window_lead_315641373767519, local._virt_window_lead_3949607449893123, local._virt_window_lead_5067641372653397, local._virt_window_lead_5767709179323528, local._virt_window_lead_6651551761445993, local._virt_window_lead_9809563217267406, local.friday_sales, local.monday_sales, local.saturday_sales, local.sunday_sales, local.thursday_sales, local.tuesday_sales, local.wednesday_sales`
- hidden: `-`
- predecessors: `grp:basic:d*:date.week_seq:sig:bf8c7f:lineage, grp:window:d0:date.week_seq:lineage`
- successors: `__final__:merge`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:date.id:sig:d8f8fc",
    "may_project_dimension": true,
    "parent_group_id": "grp:basic:d*:date.week_seq:sig:bf8c7f",
    "preserve_keys": [
      "date.week_seq",
      "date.id"
    ],
    "required_grain": [
      "date.week_seq",
      "date.id"
    ],
    "required_outputs": [
      "date.week_seq",
      "local.monday_sales",
      "local.tuesday_sales",
      "local.thursday_sales",
      "local.friday_sales",
      "local.sunday_sales",
      "local.saturday_sales",
      "local.wednesday_sales"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:date.id:sig:d8f8fc",
    "may_project_dimension": false,
    "parent_group_id": "grp:window:d0:date.week_seq",
    "preserve_keys": [
      "date.week_seq",
      "date.id"
    ],
    "required_grain": [
      "date.week_seq",
      "date.id"
    ],
    "required_outputs": [
      "date.week_seq",
      "local._virt_window_lead_9809563217267406",
      "local._virt_window_lead_5767709179323528",
      "local._virt_window_lead_2497781736791521",
      "local._virt_window_lead_5067641372653397",
      "local._virt_window_lead_3949607449893123",
      "local._virt_window_lead_6651551761445993",
      "local._virt_window_lead_315641373767519"
    ]
  }
]
```

## grp:basic:d*:date.week_seq:sig:bf8c7f

- derivation: `basic`
- depth: `d*`
- grain: `date.week_seq`
- aggregate input grain: `-`
- primary members: `local.friday_sales, local.monday_sales, local.saturday_sales, local.sunday_sales, local.thursday_sales, local.tuesday_sales, local.wednesday_sales`
- secondary members: `-`
- outputs: `date.week_seq, local.friday_sales, local.monday_sales, local.saturday_sales, local.sunday_sales, local.thursday_sales, local.tuesday_sales, local.wednesday_sales`
- inputs: `date.week_seq, local._virt_agg_sum_2131371712943644, local._virt_agg_sum_2293560889657522, local._virt_agg_sum_3727603509126659, local._virt_agg_sum_4518379598128005, local._virt_agg_sum_5332872165953971, local._virt_agg_sum_5446384850356435, local._virt_agg_sum_6823422966658347, local._virt_agg_sum_7224794219444244, local._virt_agg_sum_733654448721027, local._virt_agg_sum_7662941318754865, local._virt_agg_sum_8086371301050807, local._virt_agg_sum_8302396398525202, local._virt_agg_sum_8833754379564371, local._virt_agg_sum_9238538473336606`
- hidden: `-`
- predecessors: `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:lineage, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:lineage`
- successors: `__final__:merge, grp:basic:d*:date.id:sig:d8f8fc:lineage, grp:window:d0:date.week_seq:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:date.week_seq:sig:bf8c7f",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "date.week_seq",
      "local._virt_agg_sum_7662941318754865",
      "local._virt_agg_sum_3727603509126659",
      "local._virt_agg_sum_7224794219444244",
      "local._virt_agg_sum_2131371712943644",
      "local._virt_agg_sum_5446384850356435",
      "local._virt_agg_sum_733654448721027",
      "local._virt_agg_sum_4518379598128005"
    ]
  },
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:basic:d*:date.week_seq:sig:bf8c7f",
    "may_project_dimension": false,
    "parent_group_id": "grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "date.week_seq",
      "local._virt_agg_sum_6823422966658347",
      "local._virt_agg_sum_8302396398525202",
      "local._virt_agg_sum_8086371301050807",
      "local._virt_agg_sum_5332872165953971",
      "local._virt_agg_sum_2293560889657522",
      "local._virt_agg_sum_9238538473336606",
      "local._virt_agg_sum_8833754379564371"
    ]
  }
]
```

## grp:root:root:∅

- derivation: `root`
- depth: `root`
- grain: `-`
- aggregate input grain: `-`
- primary members: `catalog_sales.ext_sales_price, date.day_of_week, date.id, date.week_seq, date.year, web_sales.ext_sales_price`
- secondary members: `-`
- outputs: `catalog_sales.ext_sales_price, catalog_sales.item.id, catalog_sales.order_number, date.day_of_week, date.id, date.week_seq, date.year, web_sales.ext_sales_price, web_sales.item.id, web_sales.order_number`
- inputs: `catalog_sales.item.id, catalog_sales.order_number, web_sales.item.id, web_sales.order_number`
- hidden: `-`
- predecessors: `-`
- successors: `__final__:merge, grp:[@condition]filter:d1:date.id:sig:9dfe8e:lineage, grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id:lineage, grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number:lineage, grp:window:d0:date.week_seq:lineage`
- conditions: `BuildSubselectComparison(left=date.week_seq@Grain<date.id>, right=local.relevent_week_seq@Grain<date.id>, operator=<ComparisonOperator.IN: 'in'>)`

## grp:window:d0:date.week_seq

- derivation: `window`
- depth: `d0`
- grain: `date.week_seq`
- aggregate input grain: `-`
- primary members: `local._virt_window_lead_2497781736791521, local._virt_window_lead_315641373767519, local._virt_window_lead_3949607449893123, local._virt_window_lead_5067641372653397, local._virt_window_lead_5767709179323528, local._virt_window_lead_6651551761445993, local._virt_window_lead_9809563217267406`
- secondary members: `date.week_seq`
- outputs: `date.week_seq, local._virt_window_lead_2497781736791521, local._virt_window_lead_315641373767519, local._virt_window_lead_3949607449893123, local._virt_window_lead_5067641372653397, local._virt_window_lead_5767709179323528, local._virt_window_lead_6651551761445993, local._virt_window_lead_9809563217267406`
- inputs: `date.week_seq, local.friday_sales, local.monday_sales, local.saturday_sales, local.sunday_sales, local.thursday_sales, local.tuesday_sales, local.wednesday_sales`
- hidden: `-`
- predecessors: `grp:basic:d*:date.week_seq:sig:bf8c7f:lineage, grp:root:root:∅:lineage`
- successors: `__final__:merge, grp:basic:d*:date.id:sig:d8f8fc:lineage`
- input contracts:
```json
[
  {
    "channel": "row_stream",
    "consumer_group_id": "grp:window:d0:date.week_seq",
    "may_project_dimension": true,
    "parent_group_id": "grp:basic:d*:date.week_seq:sig:bf8c7f",
    "preserve_keys": [
      "date.week_seq"
    ],
    "required_grain": [
      "date.week_seq"
    ],
    "required_outputs": [
      "date.week_seq",
      "local.monday_sales",
      "local.tuesday_sales",
      "local.thursday_sales",
      "local.friday_sales",
      "local.sunday_sales",
      "local.saturday_sales",
      "local.wednesday_sales"
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

- `grp:[@condition]filter:d1:date.id:sig:9dfe8e` -> `__final__` kind=merge
- `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` -> `__final__` kind=merge
- `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` -> `grp:basic:d*:date.week_seq:sig:bf8c7f` kind=lineage
- `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` -> `__final__` kind=merge
- `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` -> `grp:basic:d*:date.week_seq:sig:bf8c7f` kind=lineage
- `grp:basic:d*:date.id:sig:d8f8fc` -> `__final__` kind=merge
- `grp:basic:d*:date.week_seq:sig:bf8c7f` -> `__final__` kind=merge
- `grp:basic:d*:date.week_seq:sig:bf8c7f` -> `grp:basic:d*:date.id:sig:d8f8fc` kind=lineage
- `grp:basic:d*:date.week_seq:sig:bf8c7f` -> `grp:window:d0:date.week_seq` kind=lineage
- `grp:root:root:∅` -> `__final__` kind=merge
- `grp:root:root:∅` -> `grp:[@condition]filter:d1:date.id:sig:9dfe8e` kind=lineage
- `grp:root:root:∅` -> `grp:aggregate:d0:date.week_seq:input:catalog_sales.item.id|catalog_sales.order_number|date.id` kind=lineage
- `grp:root:root:∅` -> `grp:aggregate:d0:date.week_seq:input:date.id|web_sales.item.id|web_sales.order_number` kind=lineage
- `grp:root:root:∅` -> `grp:window:d0:date.week_seq` kind=lineage
- `grp:window:d0:date.week_seq` -> `__final__` kind=merge
- `grp:window:d0:date.week_seq` -> `grp:basic:d*:date.id:sig:d8f8fc` kind=lineage
