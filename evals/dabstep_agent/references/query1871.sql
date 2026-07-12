-- DABstep task 1871 (hard): In January 2023 what delta would
-- Belles_cookbook_store pay if the relative fee (rate) of the fee with ID=384
-- changed to 1? Published answer: -0.94810300000017 (ours matches to float
-- noise: -0.948103). delta = sum over January transactions matched by fee 384
-- of (new_rate - old_rate) * amount / 10000. Same matching semantics as
-- query1681.sql.
WITH txn AS (
    SELECT p.*,
        month(make_date(p.year, 1, 1) + INTERVAL ((p.day_of_year - 1)) DAY) AS month,
        (p.issuing_country = p.acquirer_country) AS intracountry
    FROM payments p
    WHERE p.merchant = 'Belles_cookbook_store'
),
monthly AS (
    SELECT month,
        sum(eur_amount) AS volume,
        100.0 * sum(CASE WHEN has_fraudulent_dispute THEN eur_amount ELSE 0 END)
            / sum(eur_amount) AS fraud_pct
    FROM txn GROUP BY month
),
m AS (SELECT * FROM merchant_data WHERE merchant = 'Belles_cookbook_store'),
matched AS (
    SELECT t.*, f.ID AS fee_id
    FROM txn t
    JOIN monthly mo ON mo.month = t.month
    CROSS JOIN m
    JOIN fees f
      ON f.card_scheme = t.card_scheme
     AND (f.is_credit IS NULL OR f.is_credit = t.is_credit)
     AND (f.intracountry IS NULL OR f.intracountry = t.intracountry)
     AND (NOT EXISTS (SELECT 1 FROM fee_account_types a WHERE a.fee_id = f.ID)
          OR EXISTS (SELECT 1 FROM fee_account_types a
                     WHERE a.fee_id = f.ID AND a.account_type = m.account_type))
     AND (NOT EXISTS (SELECT 1 FROM fee_merchant_category_codes c WHERE c.fee_id = f.ID)
          OR EXISTS (SELECT 1 FROM fee_merchant_category_codes c
                     WHERE c.fee_id = f.ID AND c.mcc = m.merchant_category_code))
     AND (NOT EXISTS (SELECT 1 FROM fee_acis x WHERE x.fee_id = f.ID)
          OR EXISTS (SELECT 1 FROM fee_acis x
                     WHERE x.fee_id = f.ID AND x.aci = t.aci))
     AND (f.capture_delay IS NULL
          OR f.capture_delay = m.capture_delay
          OR (f.capture_delay = '<3' AND m.capture_delay NOT IN ('immediate','manual')
              AND CAST(m.capture_delay AS INTEGER) < 3)
          OR (f.capture_delay = '3-5' AND m.capture_delay NOT IN ('immediate','manual')
              AND CAST(m.capture_delay AS INTEGER) BETWEEN 3 AND 5)
          OR (f.capture_delay = '>5' AND m.capture_delay NOT IN ('immediate','manual')
              AND CAST(m.capture_delay AS INTEGER) > 5))
     AND (f.monthly_volume IS NULL
          OR (f.monthly_volume = '<100k' AND mo.volume < 100000)
          OR (f.monthly_volume = '100k-1m' AND mo.volume BETWEEN 100000 AND 1000000)
          OR (f.monthly_volume = '1m-5m' AND mo.volume BETWEEN 1000000 AND 5000000)
          OR (f.monthly_volume = '>5m' AND mo.volume > 5000000))
     AND (f.monthly_fraud_level IS NULL
          OR (f.monthly_fraud_level = '<7.2%' AND mo.fraud_pct < 7.2)
          OR (f.monthly_fraud_level = '7.2%-7.7%' AND mo.fraud_pct BETWEEN 7.2 AND 7.7)
          OR (f.monthly_fraud_level = '7.7%-8.3%' AND mo.fraud_pct BETWEEN 7.7 AND 8.3)
          OR (f.monthly_fraud_level = '>8.3%' AND mo.fraud_pct > 8.3))
)
SELECT sum((1 - f2.rate) * matched.eur_amount / 10000.0) AS delta
FROM matched
JOIN fees f2 ON f2.ID = matched.fee_id
WHERE month = 1 AND year = 2023 AND fee_id = 384
