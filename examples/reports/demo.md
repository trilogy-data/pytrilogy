# Quarterly Sales Report

This entire document — prose, tables, and charts — is generated from a single
markdown file with embedded Trilogy. Run it with:

```
trilogy render examples/reports/demo.md --to png
```

The block below declares the data model. Setup blocks produce no output, so
they disappear from the rendered report.

```trilogy
import std.currency;
import std.display;
key region string;
property region.revenue float::usd;
property region.units int;

auto total_revenue <-sum(revenue);
auto total_units <-sum(units);

datasource sales (
    r: region,
    rev: revenue,
    u: units
)
grain (region)
query '''
select 'North' as r, 120000.0 as rev, 340 as u
union all select 'South', 98000.0, 280
union all select 'East', 143000.0, 410
union all select 'West', 76000.0, 210
''';
```
## Headline metrics

A `headline` chart prints its `x_axis` value as a large number — bind an
aggregate so it resolves to a single KPI figure.

:::row
```trilogy
chart
  layer headline ( x_axis <- total_revenue );
```
```trilogy
chart
  layer headline ( x_axis <- total_units );
```
```trilogy
chart
  layer headline ( x_axis <- sum(revenue)/sum(units) as avg_unit_rev );
```
:::

## Revenue by region

The table below is the output of a Trilogy `select` — the source statement is
replaced by its result.

```trilogy
select region, revenue, units
order by revenue desc;
```

## Revenue, visualized

And the same figures rendered as a chart statement:

```trilogy
chart
  layer bar ( x_axis <- region, y_axis <- revenue );
```

## Side by side

Charts inside a `:::row` container render in a single row:

:::row
```trilogy
chart layer barh ( x_axis <- revenue, y_axis <- region );
```
```trilogy
chart layer bar ( x_axis <- region, y_axis <- revenue );
```
:::

Everything above was authored as plain markdown — an agent can write one of
these and hand back a polished report.
