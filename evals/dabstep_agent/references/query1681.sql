-- DABstep task 1681 (hard): For the 10th of the year 2023 (day_of_year = 10),
-- what are the Fee IDs applicable to Belles_cookbook_store?
-- Published answer: 741, 709, 454, 813, 381, 536, 473, 572, 477, 286
-- (validated exact).
-- A fee applies to a transaction when every populated rule field matches the
-- transaction/merchant; empty child tables and NULL scalars = applies to all.
-- Monthly volume / fraud level are computed per natural month over the
-- merchant's full transaction history (fraud = fraudulent volume share).
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
SELECT DISTINCT fee_id
FROM matched
WHERE day_of_year = 10 AND year = 2023
