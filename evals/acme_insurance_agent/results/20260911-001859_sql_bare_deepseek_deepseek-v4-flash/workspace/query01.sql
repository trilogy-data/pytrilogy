SELECT pol.Policy_Number,
       SUM(pa.Policy_Amount) AS total_premium_paid
FROM Premium prem
JOIN Policy_Amount pa ON pa.Policy_Amount_Identifier = prem.Policy_Amount_Identifier
JOIN Policy pol ON pol.Policy_Identifier = pa.Policy_Identifier
GROUP BY pol.Policy_Number
ORDER BY pol.Policy_Number;
