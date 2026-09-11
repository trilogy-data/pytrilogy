SELECT SUM(pa.Policy_Amount) / COUNT(DISTINCT pa.Policy_Identifier) AS average_policy_size
FROM Premium pr
JOIN Policy_Amount pa
  ON pr.Policy_Amount_Identifier = pa.Policy_Amount_Identifier
