-- DABstep task 1273 (hard): For credit transactions, what would be the average
-- fee that the card scheme GlobalCard would charge for a transaction value of
-- 10 EUR? Published answer: 0.120132.
-- fee = fixed_amount + rate * value / 10000; NULL is_credit = applies to all,
-- so credit transactions match rules with is_credit true OR null.
SELECT avg(fixed_amount + rate * 10.0 / 10000) AS avg_fee
FROM fees
WHERE card_scheme = 'GlobalCard'
  AND (is_credit IS NULL OR is_credit = true)
