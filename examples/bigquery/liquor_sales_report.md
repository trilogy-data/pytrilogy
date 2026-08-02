# Top Liquor Sales by Year

## Latest five years: 2022–2026

This report ranks the **top 10 liquor products by sales dollars within each year**. The latest year available is 2026; it may be a partial year. Bottle counts are included as a volume reference.

```trilogy
import raw.bigquery_public_data_iowa_liquor_sales_sales as sales;

auto sales_year <- year(sales.date);
auto total_sales_dollars <- sum(sales.sale_dollars);
auto bottles_sold <- sum(sales.bottles_sold);
auto latest_year <- max(year(sales.date)) by *;
auto sales_rank <- rank(sales.item_description) over (partition by sales_year order by total_sales_dollars desc);
```

## Annual leaders

```trilogy
where sales_year >= latest_year - 4
select
    sales_year,
    sales.item_description,
    total_sales_dollars,
    bottles_sold,
    sales_rank
having sales_rank = 1
order by sales_year desc;
```

## Top 10 products in each year

```trilogy
where sales_year >= latest_year - 4
select
    sales_year,
    sales.item_description,
    total_sales_dollars,
    bottles_sold,
    sales_rank
having sales_rank <= 10
order by sales_year desc, sales_rank asc;
```

## Sales-dollar comparison

```trilogy
chart
  set show_title
  layer bar (
    x_axis <- sales.item_description,
    y_axis <- total_sales_dollars,
    color <- sales_year,
    x_trellis <- sales_year
  )
  from where sales_year >= latest_year - 4
  select sales_year, sales.item_description, total_sales_dollars, sales_rank
  having sales_rank <= 10
  order by sales_rank asc;
```

*Source: Iowa Liquor Sales public dataset. “Top” is defined by summed `sale_dollars`; returns or adjustments in the source are retained.*
