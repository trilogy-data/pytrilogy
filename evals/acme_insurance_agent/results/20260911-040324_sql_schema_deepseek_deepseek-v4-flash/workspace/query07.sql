SELECT
    p.Policy_Number,
    SUM(pa.Policy_Amount) AS Total_Premium_Paid
FROM Premium pr
JOIN Policy_Amount pa
    ON pa.Policy_Amount_Identifier = pr.Policy_Amount_Identifier
JOIN Policy p
    ON p.Policy_Identifier = pa.Policy_Identifier
GROUP BY p.Policy_Number
ORDER BY p.Policy_Number
