CREATE OR REPLACE TABLE dim_hr_department AS
SELECT i AS department_id, 'Department ' || i AS department_name,
       1000 + i AS cost_center_id, current_date - i::INTEGER AS opened_date
FROM range(1, 26) t(i);

CREATE OR REPLACE TABLE dim_hr_employee AS
SELECT i AS employee_id, 1 + (i % 25) AS department_id,
       'Employee ' || i AS display_name, 'active' AS employment_status,
       current_date - (i % 2500)::INTEGER AS hire_date,
       50000 + (i % 80) * 1000 AS annual_salary
FROM range(1, 501) t(i);

CREATE OR REPLACE TABLE fact_payroll_entry AS
SELECT i AS payroll_entry_id, 1 + (i % 500) AS employee_id,
       current_date - (i % 365)::INTEGER AS pay_date,
       1800 + (i % 700) AS gross_amount, 400 + (i % 200) AS tax_amount,
       'USD' AS currency_code
FROM range(1, 2001) t(i);

CREATE OR REPLACE TABLE fact_support_ticket AS
SELECT i AS support_ticket_id, 100000 + (i % 800) AS account_id,
       CASE i % 4 WHEN 0 THEN 'open' WHEN 1 THEN 'pending' ELSE 'closed' END AS status,
       CASE i % 3 WHEN 0 THEN 'billing' WHEN 1 THEN 'technical' ELSE 'general' END AS queue,
       current_timestamp - INTERVAL (i % 720) HOUR AS created_at,
       'Support request ' || i AS subject
FROM range(1, 1201) t(i);

CREATE OR REPLACE TABLE fact_support_ticket_event AS
SELECT i AS ticket_event_id, 1 + (i % 1200) AS support_ticket_id,
       current_timestamp - INTERVAL (i % 1440) MINUTE AS event_at,
       CASE i % 3 WHEN 0 THEN 'comment' WHEN 1 THEN 'assignment' ELSE 'status_change' END AS event_type,
       1 + (i % 80) AS actor_id
FROM range(1, 5001) t(i);

CREATE OR REPLACE TABLE dim_marketing_campaign AS
SELECT i AS campaign_id, 'Campaign ' || i AS campaign_name,
       current_date - (i * 10)::INTEGER AS start_date,
       current_date + (30 - i)::INTEGER AS end_date,
       10000 + i * 750 AS planned_budget,
       CASE i % 3 WHEN 0 THEN 'email' WHEN 1 THEN 'social' ELSE 'search' END AS channel
FROM range(1, 41) t(i);

CREATE OR REPLACE TABLE fact_marketing_touch AS
SELECT i AS touch_id, 1 + (i % 40) AS campaign_id,
       200000 + (i % 4000) AS contact_id,
       current_timestamp - INTERVAL (i % 90) DAY AS touched_at,
       CASE i % 4 WHEN 0 THEN 'click' WHEN 1 THEN 'view' ELSE 'delivered' END AS outcome
FROM range(1, 10001) t(i);

CREATE OR REPLACE TABLE dim_supplier_contract AS
SELECT i AS contract_id, 3000 + (i % 120) AS supplier_id,
       'Contract ' || i AS contract_name,
       current_date - (i % 1000)::INTEGER AS effective_date,
       current_date + (i % 800)::INTEGER AS expiration_date,
       25000 + i * 125 AS committed_value,
       CASE i % 3 WHEN 0 THEN 'active' ELSE 'review' END AS contract_status
FROM range(1, 301) t(i);

CREATE OR REPLACE TABLE dim_fleet_vehicle AS
SELECT i AS vehicle_id, 'VIN' || lpad(i::VARCHAR, 12, '0') AS vin,
       2015 + (i % 11) AS model_year, 'Depot ' || (i % 8) AS home_depot,
       CASE i % 4 WHEN 0 THEN 'service' ELSE 'available' END AS vehicle_status
FROM range(1, 151) t(i);

CREATE OR REPLACE TABLE fact_fleet_maintenance AS
SELECT i AS maintenance_id, 1 + (i % 150) AS vehicle_id,
       current_date - (i % 600)::INTEGER AS service_date,
       CASE i % 3 WHEN 0 THEN 'inspection' WHEN 1 THEN 'repair' ELSE 'oil_change' END AS service_type,
       75 + (i % 900) AS service_cost, 10000 + (i % 140000) AS odometer_reading
FROM range(1, 1001) t(i);

CREATE OR REPLACE TABLE dim_project_milestone AS
SELECT i AS milestone_id, 900 + (i % 60) AS project_id,
       'Milestone ' || i AS milestone_name,
       current_date + (i % 365)::INTEGER AS target_date,
       CASE i % 3 WHEN 0 THEN 'complete' ELSE 'planned' END AS milestone_status,
       1 + (i % 500) AS owner_employee_id
FROM range(1, 401) t(i);

CREATE OR REPLACE TABLE fact_application_audit_event AS
SELECT i AS audit_event_id, current_timestamp - INTERVAL (i % 10000) SECOND AS event_at,
       1 + (i % 500) AS actor_employee_id,
       CASE i % 4 WHEN 0 THEN 'login' WHEN 1 THEN 'export' ELSE 'update' END AS action,
       'object-' || (i % 700) AS object_identifier,
       CASE i % 5 WHEN 0 THEN 'denied' ELSE 'success' END AS result
FROM range(1, 15001) t(i);

-- Descriptions matching the enrichment style of comments.sql: honest domain
-- summaries only, no relevance hints.
COMMENT ON TABLE dim_hr_department IS 'Internal HR department reference data.';
COMMENT ON TABLE dim_hr_employee IS 'Internal HR employee roster.';
COMMENT ON COLUMN dim_hr_employee.annual_salary IS 'USD.';
COMMENT ON TABLE fact_payroll_entry IS 'Internal payroll entries, one row per employee pay event.';
COMMENT ON COLUMN fact_payroll_entry.gross_amount IS 'Gross pay for the period (USD).';
COMMENT ON TABLE fact_support_ticket IS 'Customer support tickets, one row per ticket.';
COMMENT ON TABLE fact_support_ticket_event IS 'Event log for support tickets (comments, assignments, status changes).';
COMMENT ON TABLE dim_marketing_campaign IS 'Marketing campaign reference data.';
COMMENT ON COLUMN dim_marketing_campaign.planned_budget IS 'USD.';
COMMENT ON TABLE fact_marketing_touch IS 'Marketing contact touches (delivered / view / click outcomes).';
COMMENT ON TABLE dim_supplier_contract IS 'Supplier contract reference data.';
COMMENT ON COLUMN dim_supplier_contract.committed_value IS 'USD.';
COMMENT ON TABLE dim_fleet_vehicle IS 'Company fleet vehicle registry.';
COMMENT ON TABLE fact_fleet_maintenance IS 'Fleet vehicle service events.';
COMMENT ON COLUMN fact_fleet_maintenance.service_cost IS 'USD.';
COMMENT ON COLUMN fact_fleet_maintenance.odometer_reading IS 'Miles at time of service.';
COMMENT ON TABLE dim_project_milestone IS 'Internal project milestone tracker.';
COMMENT ON TABLE fact_application_audit_event IS 'Application audit log (logins, exports, updates).';
