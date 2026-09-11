SELECT p.Policy_Number, SUM(pa.Policy_Amount) AS total_premium
FROM Premium pr
JOIN Policy_Amount pa ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
JOIN Policy p ON pa.Policy_Identifier = p.Policy_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number;
