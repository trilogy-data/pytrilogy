-- Table/column descriptions for the messy-warehouse SQL legs, applied as DuckDB
-- COMMENT ON metadata and surfaced through the generated schema.md. Mirrors the
-- enrichment carried by the curated Trilogy models (tests/modeling/tpc_ds_duckdb)
-- so sql_schema_* and enriched_* variants see roughly equivalent documentation.

COMMENT ON TABLE fact_store_sales IS 'Store-channel sales lines, one row per (ss_ticket_number, ss_item_sk). Count line items with count(*), not count(distinct ss_ticket_number): a multi-item receipt shares one ticket. Returns are in fact_store_returns, matched on item + ticket.';
COMMENT ON COLUMN fact_store_sales.ss_sales_price IS 'Per-unit price charged; the whole-line total is ss_ext_sales_price.';
COMMENT ON COLUMN fact_store_sales.ss_ext_sales_price IS 'Whole-line total = ss_sales_price * ss_quantity.';
COMMENT ON COLUMN fact_store_sales.ss_list_price IS 'Per-unit, before discounts.';
COMMENT ON COLUMN fact_store_sales.ss_wholesale_cost IS 'Per-unit.';
COMMENT ON COLUMN fact_store_sales.ss_net_paid IS 'Excludes tax.';
COMMENT ON COLUMN fact_store_sales.ss_coupon_amt IS 'Coupon/credit applied to the line.';
COMMENT ON COLUMN fact_store_sales.ss_customer_sk IS 'Nullable: anonymous sales allowed.';
COMMENT ON COLUMN fact_store_sales.ss_cdemo_sk IS 'Buyer demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_store_sales.ss_hdemo_sk IS 'Buyer household demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_store_sales.ss_addr_sk IS 'Customer address as of the sale, not the customer''s current address.';

COMMENT ON TABLE fact_store_returns IS 'Store-channel returns, one row per (sr_ticket_number, sr_item_sk), matching fact_store_sales lines; LEFT JOIN from sales on both keys to identify returned lines.';
COMMENT ON COLUMN fact_store_returns.sr_customer_sk IS 'Returning customer; may differ from the purchaser. Nullable.';
COMMENT ON COLUMN fact_store_returns.sr_cdemo_sk IS 'Returner demographics as of the return, not the customer''s current record.';
COMMENT ON COLUMN fact_store_returns.sr_hdemo_sk IS 'Returner household demographics as of the return, not the customer''s current record.';
COMMENT ON COLUMN fact_store_returns.sr_addr_sk IS 'Returner address as of the return, not the customer''s current address.';
COMMENT ON COLUMN fact_store_returns.sr_return_amt IS 'Refund excluding tax.';
COMMENT ON COLUMN fact_store_returns.sr_fee IS 'Restocking/handling fee withheld from the refund.';
COMMENT ON COLUMN fact_store_returns.sr_reversed_charge IS 'Refund portion reversed on the original payment method.';
COMMENT ON COLUMN fact_store_returns.sr_store_sk IS 'Store the return was processed at; nullable.';

COMMENT ON TABLE fact_catalog_sales IS 'Catalog-channel sales lines, one row per (cs_order_number, cs_item_sk). Count line items with count(*), not count(distinct cs_order_number): a multi-item order shares one order number. Returns are in fact_catalog_returns, matched on item + order number.';
COMMENT ON COLUMN fact_catalog_sales.cs_sales_price IS 'Per-unit price charged; the whole-line total is cs_ext_sales_price.';
COMMENT ON COLUMN fact_catalog_sales.cs_ext_sales_price IS 'Whole-line total = cs_sales_price * cs_quantity.';
COMMENT ON COLUMN fact_catalog_sales.cs_list_price IS 'Per-unit, before discounts.';
COMMENT ON COLUMN fact_catalog_sales.cs_wholesale_cost IS 'Per-unit.';
COMMENT ON COLUMN fact_catalog_sales.cs_net_paid IS 'Excludes tax/shipping.';
COMMENT ON COLUMN fact_catalog_sales.cs_coupon_amt IS 'Coupon/credit applied to the line.';
COMMENT ON COLUMN fact_catalog_sales.cs_bill_cdemo_sk IS 'Buyer demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_catalog_sales.cs_bill_hdemo_sk IS 'Buyer household demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_catalog_sales.cs_bill_addr_sk IS 'Billing address recorded on the sale at point of sale, not the customer''s current address.';
COMMENT ON COLUMN fact_catalog_sales.cs_ship_addr_sk IS 'Shipping address recorded on the sale at point of sale, not the customer''s current address.';

COMMENT ON TABLE fact_catalog_returns IS 'Catalog-channel returns, one row per (cr_order_number, cr_item_sk), matching fact_catalog_sales lines; LEFT JOIN from sales on both keys to identify returned lines.';
COMMENT ON COLUMN fact_catalog_returns.cr_returning_customer_sk IS 'Customer who returned the item.';
COMMENT ON COLUMN fact_catalog_returns.cr_refunded_customer_sk IS 'Customer who received the refund; may differ from the returning customer.';
COMMENT ON COLUMN fact_catalog_returns.cr_returning_cdemo_sk IS 'Returning customer''s demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_catalog_returns.cr_refunded_cdemo_sk IS 'Refunded customer''s demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_catalog_returns.cr_returning_hdemo_sk IS 'Returning customer''s household demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_catalog_returns.cr_refunded_hdemo_sk IS 'Refunded customer''s household demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_catalog_returns.cr_returning_addr_sk IS 'Address the return originated from.';
COMMENT ON COLUMN fact_catalog_returns.cr_refunded_addr_sk IS 'Address the refund was issued to.';
COMMENT ON COLUMN fact_catalog_returns.cr_return_amount IS 'Refund excluding tax.';
COMMENT ON COLUMN fact_catalog_returns.cr_fee IS 'Restocking/handling fee withheld from the refund.';
COMMENT ON COLUMN fact_catalog_returns.cr_reversed_charge IS 'Refund portion reversed on the original payment method.';
COMMENT ON COLUMN fact_catalog_returns.cr_warehouse_sk IS 'Warehouse the item was returned to.';

COMMENT ON TABLE fact_web_sales IS 'Web-channel sales lines, one row per (ws_order_number, ws_item_sk). Count line items with count(*), not count(distinct ws_order_number): a multi-item order shares one order number. Returns are in fact_web_returns, matched on item + order number.';
COMMENT ON COLUMN fact_web_sales.ws_sales_price IS 'Per-unit price charged; the whole-line total is ws_ext_sales_price.';
COMMENT ON COLUMN fact_web_sales.ws_ext_sales_price IS 'Whole-line total = ws_sales_price * ws_quantity.';
COMMENT ON COLUMN fact_web_sales.ws_list_price IS 'Per-unit, before discounts.';
COMMENT ON COLUMN fact_web_sales.ws_wholesale_cost IS 'Per-unit.';
COMMENT ON COLUMN fact_web_sales.ws_net_paid IS 'Excludes tax/shipping.';
COMMENT ON COLUMN fact_web_sales.ws_coupon_amt IS 'Coupon/credit applied to the line.';
COMMENT ON COLUMN fact_web_sales.ws_bill_cdemo_sk IS 'Bill-to customer demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_web_sales.ws_ship_cdemo_sk IS 'Ship-to customer demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_web_sales.ws_bill_hdemo_sk IS 'Bill-to household demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_web_sales.ws_ship_hdemo_sk IS 'Ship-to household demographics as of the sale, not the customer''s current record.';
COMMENT ON COLUMN fact_web_sales.ws_bill_addr_sk IS 'Billing address recorded on the sale at point of sale, not the customer''s current address.';
COMMENT ON COLUMN fact_web_sales.ws_ship_addr_sk IS 'Shipping address recorded on the sale at point of sale, not the customer''s current address.';

COMMENT ON TABLE fact_web_returns IS 'Web-channel returns, one row per (wr_order_number, wr_item_sk), matching fact_web_sales lines; LEFT JOIN from sales on both keys to identify returned lines.';
COMMENT ON COLUMN fact_web_returns.wr_returning_customer_sk IS 'Customer who returned the item.';
COMMENT ON COLUMN fact_web_returns.wr_refunded_customer_sk IS 'Customer who received the refund; may differ from the returning customer.';
COMMENT ON COLUMN fact_web_returns.wr_returning_cdemo_sk IS 'Returning customer''s demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_web_returns.wr_refunded_cdemo_sk IS 'Refunded customer''s demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_web_returns.wr_returning_hdemo_sk IS 'Returning customer''s household demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_web_returns.wr_refunded_hdemo_sk IS 'Refunded customer''s household demographics as of the return, not their current record.';
COMMENT ON COLUMN fact_web_returns.wr_returning_addr_sk IS 'Address the return originated from.';
COMMENT ON COLUMN fact_web_returns.wr_refunded_addr_sk IS 'Address the refund was issued to.';
COMMENT ON COLUMN fact_web_returns.wr_return_amt IS 'Refund excluding tax.';
COMMENT ON COLUMN fact_web_returns.wr_fee IS 'Restocking/handling fee withheld from the refund.';
COMMENT ON COLUMN fact_web_returns.wr_reversed_charge IS 'Refund portion reversed on the original payment method.';
COMMENT ON COLUMN fact_web_returns.wr_web_page_sk IS 'Web page the return was filed through.';

COMMENT ON TABLE fact_inventory IS 'Inventory snapshots: quantity on hand per (inv_date_sk, inv_item_sk, inv_warehouse_sk); snapshot dates are typically weekly.';

COMMENT ON COLUMN dim_date_dim.d_dow IS '0 = Sunday ... 6 = Saturday.';
COMMENT ON COLUMN dim_date_dim.d_week_seq IS 'Monotonic week-of-time sequence, ~53 per year (same week one year later is d_week_seq + 53). One d_week_seq can span two years.';
COMMENT ON COLUMN dim_date_dim.d_month_seq IS 'Monotonic month-of-time sequence.';
COMMENT ON COLUMN dim_date_dim.d_qoy IS 'Use this for quarters, not quarter(d_date): the dim''s quarters do not always match calendar quarters.';
COMMENT ON COLUMN dim_date_dim.d_quarter_name IS 'e.g. ''1998Q3''.';

COMMENT ON TABLE dim_item IS 'Slowly changing dimension: multiple i_item_sk rows per i_item_id.';
COMMENT ON COLUMN dim_item.i_item_id IS 'Business item code, the typical per-item identifier. NOT unique (SCD): use for per-item results, not i_item_sk.';
COMMENT ON COLUMN dim_item.i_brand_id IS 'Composite of category + class + brand digits.';
COMMENT ON COLUMN dim_item.i_class_id IS 'Within-category id; not globally unique.';
COMMENT ON COLUMN dim_item.i_current_price IS 'Current list price.';
COMMENT ON COLUMN dim_item.i_manager_id IS 'Merchandise manager responsible for the item.';
COMMENT ON COLUMN dim_item.i_units IS 'Unit of measure, e.g. ''Each'', ''Ounce'', ''Pound''.';

COMMENT ON COLUMN dim_customer.c_current_cdemo_sk IS 'Current record; sales/returns facts carry the as-of-sale keys instead.';
COMMENT ON COLUMN dim_customer.c_current_hdemo_sk IS 'Current record; sales/returns facts carry the as-of-sale keys instead.';
COMMENT ON COLUMN dim_customer.c_current_addr_sk IS 'Current record; sales/returns facts carry the as-of-sale keys instead.';
COMMENT ON COLUMN dim_customer.c_birth_country IS 'Free-form, e.g. ''UNITED STATES''.';

COMMENT ON COLUMN dim_customer_demographics.cd_marital_status IS 'M=married, S=single, D=divorced, W=widowed, U=unknown.';
COMMENT ON COLUMN dim_customer_demographics.cd_purchase_estimate IS 'Modeled annual purchase estimate, bucketed in $500 increments (500-10000).';

COMMENT ON COLUMN dim_household_demographics.hd_income_band_sk IS 'FK to dim_income_band.';
COMMENT ON COLUMN dim_household_demographics.hd_buy_potential IS 'Estimated annual spend bucket (USD).';
COMMENT ON COLUMN dim_household_demographics.hd_vehicle_count IS '-1 = unknown.';

COMMENT ON TABLE dim_income_band IS 'Household income ranges; ib_lower_bound and ib_upper_bound are inclusive dollar bounds.';

COMMENT ON TABLE dim_customer_address IS 'Addresses for customers, stores, warehouses, and web sites.';
COMMENT ON COLUMN dim_customer_address.ca_country IS 'Free-form, typically ''United States''.';
COMMENT ON COLUMN dim_customer_address.ca_location_type IS '''apartment'', ''condo'', or ''single family''.';
COMMENT ON COLUMN dim_customer_address.ca_gmt_offset IS 'Hours, e.g. -5.0.';

COMMENT ON COLUMN dim_store.s_store_id IS 'Business store code; report/group by this for "store code" questions.';

COMMENT ON COLUMN dim_time_dim.t_time_sk IS 'Equals seconds since midnight.';
COMMENT ON COLUMN dim_time_dim.t_time IS 'Seconds since midnight (0-86399).';
COMMENT ON COLUMN dim_time_dim.t_meal_time IS '''breakfast''/''lunch''/''dinner''; empty string for off-meal hours.';

COMMENT ON COLUMN dim_ship_mode.sm_type IS 'Delivery-speed tier.';
COMMENT ON COLUMN dim_ship_mode.sm_carrier IS 'e.g. ''UPS'', ''FEDEX'', ''USPS''.';

COMMENT ON COLUMN dim_reason.r_reason_desc IS 'Free-form, e.g. ''Did not fit'', ''wrong color''.';

COMMENT ON COLUMN dim_promotion.p_channel_dmail IS 'Direct mail (Y/N).';

COMMENT ON TABLE fact_agg_store_sales_daily IS 'Pre-aggregation of fact_store_sales grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying sales lines. Ticket-level detail is not preserved.';
COMMENT ON TABLE fact_agg_catalog_sales_daily IS 'Pre-aggregation of fact_catalog_sales grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying sales lines. Order-level detail is not preserved.';
COMMENT ON TABLE fact_agg_web_sales_daily IS 'Pre-aggregation of fact_web_sales grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying sales lines. Order-level detail is not preserved.';
COMMENT ON TABLE fact_agg_store_returns_daily IS 'Pre-aggregation of fact_store_returns grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying return lines. Ticket-level detail is not preserved.';
COMMENT ON TABLE fact_agg_catalog_returns_daily IS 'Pre-aggregation of fact_catalog_returns grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying return lines. Order-level detail is not preserved.';
COMMENT ON TABLE fact_agg_web_returns_daily IS 'Pre-aggregation of fact_web_returns grouped by its *_sk dimension columns; count_*/sum_* summarize the underlying return lines. Order-level detail is not preserved.';
