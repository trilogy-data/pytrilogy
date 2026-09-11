SELECT SUM(pa.Policy_Amount) / (SELECT COUNT(*) FROM Policy) AS average_policy_size
FROM Policy_Amount pa
JOIN Premium p ON pa.Policy_Amount_Identifier = p.Policy_Amount_Identifier;
