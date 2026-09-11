SELECT
    SUM(pa.Policy_Amount) / COUNT(DISTINCT pa.Policy_Identifier) AS average_policy_size
FROM Premium p
JOIN Policy_Amount pa
    ON p.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
WHERE pa.Policy_Identifier IS NOT NULL;
