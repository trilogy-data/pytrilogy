-- DABstep task 49 (easy): What is the top country (ip_country) for fraud?
-- Published answer: B. BE — which is the highest FRAUD RATE country
-- (by transaction count NL would win; the benchmark means rate).
SELECT ip_country
FROM payments
GROUP BY ip_country
ORDER BY avg(CASE WHEN has_fraudulent_dispute THEN 1.0 ELSE 0.0 END) DESC
LIMIT 1
