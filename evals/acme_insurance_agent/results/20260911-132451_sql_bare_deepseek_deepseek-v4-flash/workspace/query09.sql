SELECT SUM(pa.Policy_Amount) / COUNT(DISTINCT pa.Policy_Identifier) AS avg_policy_size
FROM Policy_Amount pa
JOIN Premium p ON pa.Policy_Amount_Identifier = p.Policy_Amount_Identifier