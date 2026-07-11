-- DABstep task 1305 (hard): For account type H and the MCC description
-- 'Eating Places and Restaurants', what would be the average fee that the card
-- scheme GlobalCard would charge for a transaction value of 10 EUR?
-- Published answer: 0.123217.
-- No child rows in fee_account_types / fee_merchant_category_codes = rule is
-- unconstrained on that dimension (applies to all).
SELECT avg(fixed_amount + rate * 10.0 / 10000) AS avg_fee
FROM fees f
WHERE card_scheme = 'GlobalCard'
  AND (NOT EXISTS (SELECT 1 FROM fee_account_types a WHERE a.fee_id = f.ID)
       OR EXISTS (SELECT 1 FROM fee_account_types a
                  WHERE a.fee_id = f.ID AND a.account_type = 'H'))
  AND (NOT EXISTS (SELECT 1 FROM fee_merchant_category_codes m WHERE m.fee_id = f.ID)
       OR EXISTS (SELECT 1 FROM fee_merchant_category_codes m
                  WHERE m.fee_id = f.ID
                    AND m.mcc IN (SELECT mcc FROM merchant_category_codes
                                  WHERE description = 'Eating Places and Restaurants')))
