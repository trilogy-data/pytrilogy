-- DABstep task 2697 (hard): For Belles_cookbook_store in January, if we moved
-- the fraudulent transactions to a different ACI, what would be the preferred
-- choice considering the lowest possible fees?
-- Published answer: E:13.57. The chosen ACI (E) reproduces under every
-- reasonable interpretation of the fee aggregation; the published fee value
-- 13.57 does not (16 structural variants tested — per-txn min gives 16.63,
-- per-txn mean 41.66). Our canonical definition: per transaction, average the
-- fee across all matching rules (with the transaction's ACI replaced by the
-- candidate), sum over the fraudulent January transactions, rounded to 2
-- decimals. Matching semantics as query1681.sql.
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
fraud_txn AS (
    SELECT * FROM txn WHERE month = 1 AND year = 2023 AND has_fraudulent_dispute
),
alt AS (SELECT DISTINCT aci AS alt_aci FROM fee_acis),
matched AS (
    SELECT t.psp_reference, alt.alt_aci,
           f.fixed_amount + f.rate * t.eur_amount / 10000.0 AS fee
    FROM fraud_txn t
    CROSS JOIN alt
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
                     WHERE x.fee_id = f.ID AND x.aci = alt.alt_aci))
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
),
per_txn AS (
    SELECT alt_aci, psp_reference, avg(fee) AS txn_fee
    FROM matched
    GROUP BY alt_aci, psp_reference
)
SELECT alt_aci AS preferred_aci, round(sum(txn_fee), 2) AS total_fee
FROM per_txn
GROUP BY alt_aci
ORDER BY total_fee
LIMIT 1
