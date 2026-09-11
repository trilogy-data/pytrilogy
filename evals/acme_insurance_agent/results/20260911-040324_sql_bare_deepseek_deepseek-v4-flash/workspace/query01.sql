SELECT
    p.Policy_Number,
    SUM(pa.Policy_Amount) AS total_premium_paid
FROM Policy p
JOIN Policy_Amount pa
    ON pa.Policy_Identifier = p.Policy_Identifier
JOIN Premium pr
    ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number
